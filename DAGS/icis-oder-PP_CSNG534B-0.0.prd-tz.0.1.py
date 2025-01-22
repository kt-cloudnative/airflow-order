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
                , WORKFLOW_NAME='PP_CSNG534B',WORKFLOW_ID='d072042d19f249aca096271bc52e92ee', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG534B-0.0.prd-tz.0.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 1, 12, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('d072042d19f249aca096271bc52e92ee')

    csng534bJob_vol = []
    csng534bJob_volMnt = []
    csng534bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng534bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng534bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng534bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng534bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng534bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '6f885b4033b141e0ae6eef749ffb60e2',
        'volumes': csng534bJob_vol,
        'volume_mounts': csng534bJob_volMnt,
        'env_from':csng534bJob_env,
        'task_id':'csng534bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.43',
        'arguments':["--job.name=csng534bJob", "inputDate=20240731","requestDate="+str(datetime.now())
],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('d072042d19f249aca096271bc52e92ee')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng534bJob,
        Complete
    ]) 

    # authCheck >> csng534bJob >> Complete
    workflow








