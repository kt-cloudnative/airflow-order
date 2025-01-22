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
                , WORKFLOW_NAME='PP_CSNG926B',WORKFLOW_ID='427d995f8b0049038af86d74fcff267f', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG926B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('427d995f8b0049038af86d74fcff267f')

    csng926bJob_vol = []
    csng926bJob_volMnt = []
    csng926bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng926bJob_volMnt.append(getVolumeMount('shared-volume','/app/oder'))

    csng926bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng926bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng926bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng926bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'af126d9a80f64527b6c1dc43cf57d233',
        'volumes': csng926bJob_vol,
        'volume_mounts': csng926bJob_volMnt,
        'env_from':csng926bJob_env,
        'task_id':'csng926bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.94',
        'arguments':["--job.name=csng926bJob", "tranDate=20241129", "ver=003"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('427d995f8b0049038af86d74fcff267f')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng926bJob,
        Complete
    ]) 

    # authCheck >> csng926bJob >> Complete
    workflow








