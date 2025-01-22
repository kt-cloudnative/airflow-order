from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='PP_CSNG926B',WORKFLOW_ID='22ea3bd1240a432da89d88bf3fc238de', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG926B-0.0.prd-tz.3.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 22, 15, 20, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('22ea3bd1240a432da89d88bf3fc238de')

    csng926bJob_vol = []
    csng926bJob_volMnt = []
    csng926bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng926bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng926bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng926bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng926bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng926bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '204e3e1b794a4fcbbff2c8fbe83300af',
        'volumes': csng926bJob_vol,
        'volume_mounts': csng926bJob_volMnt,
        'env_from':csng926bJob_env,
        'task_id':'csng926bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.79',
        'arguments':["job.name=csng926bJob", "tranDate=20240530"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('22ea3bd1240a432da89d88bf3fc238de')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng926bJob,
        Complete
    ]) 

    # authCheck >> csng926bJob >> Complete
    workflow








