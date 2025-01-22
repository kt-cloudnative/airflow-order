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
                , WORKFLOW_NAME='PP_CSNG125B',WORKFLOW_ID='005b57ed04a9416094ef20474c41a098', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG125B-0.0.prd-tz.6.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 29, 11, 4, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('005b57ed04a9416094ef20474c41a098')

    csng125bJob_vol = []
    csng125bJob_volMnt = []
    csng125bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng125bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng125bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng125bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng125bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng125bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'e4bc3900850b455983d9e13d59d3bd37',
        'volumes': csng125bJob_vol,
        'volume_mounts': csng125bJob_volMnt,
        'env_from':csng125bJob_env,
        'task_id':'csng125bJob',
        'image':'/icis/icis-oder-ppon-batch:0.7.1.3',
        'arguments':["--job.name=csng125bJob", "workDay=20240430", "pgmNm=csng125b", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('005b57ed04a9416094ef20474c41a098')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng125bJob,
        Complete
    ]) 

    # authCheck >> csng125bJob >> Complete
    workflow








