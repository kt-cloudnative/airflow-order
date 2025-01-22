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
                , WORKFLOW_NAME='PP_CSNG344B',WORKFLOW_ID='f136e7aa2f6a425d8303631c68953935', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-PP_CSNG344B-0.0.prd-tz.1.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 17, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('f136e7aa2f6a425d8303631c68953935')

    csng344bJob_vol = []
    csng344bJob_volMnt = []
    csng344bJob_vol.append(getVolume('shared-volume','shared-volume'))
    csng344bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    csng344bJob_env = [getICISConfigMap('icis-oder-ppon-batch-configmap'), getICISConfigMap('icis-oder-ppon-batch-configmap2'), getICISSecret('icis-oder-ppon-batch-secret')]
    csng344bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    csng344bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    csng344bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '9b1b816010754320a999ecaba6c006c0',
        'volumes': csng344bJob_vol,
        'volume_mounts': csng344bJob_volMnt,
        'env_from':csng344bJob_env,
        'task_id':'csng344bJob',
        'image':'/icis/icis-oder-ppon-batch:0.4.1.57',
        'arguments':["--job.name=csng344bJob","endTranDate=202406"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('f136e7aa2f6a425d8303631c68953935')

    workflow = COMMON.getICISPipeline([
        authCheck,
        csng344bJob,
        Complete
    ]) 

    # authCheck >> csng344bJob >> Complete
    workflow








